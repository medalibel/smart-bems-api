import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import json
import ollama
from datetime import datetime, timedelta


def generate_report(house_id,day):
    print('generating report for')
    print(house_id)
    print("at: ")
    print(day)
    # 🧠 Parameters (set dynamically later)
    HOUSE_ID    = house_id
    REPORT_DAY = day.isoformat()                  # string YYYY-MM-DD
    #report_date = pd.to_datetime(REPORT_DAY).date()
    report_date = day
    yesterday   = report_date - timedelta(days=1)

    # 📥 Load cleaned dataset
    CSV_PATH = "../data/house_3538.csv"
    WEATHER_PATH = "../data/weather_data.csv"
    df = pd.read_csv(CSV_PATH, parse_dates=["local_15min"])
    wdf = pd.read_csv(WEATHER_PATH, parse_dates=["local_15min"])
    merged_df = pd.merge(df, wdf, on='local_15min', how='left')

    merged_df["date"] = merged_df["local_15min"].dt.date
    merged_df["hour"] = merged_df["local_15min"].dt.hour


    if yesterday not in merged_df.date.values or report_date not in merged_df.date.values:
        raise ValueError("Missing data for yesterday or today")

    # 🛰️ Identify which *_present flags = 1
    present = [c for c in merged_df if c.endswith("_present") and merged_df[c].sum()>0]
    avail = {p[:-8]: p for p in present}

    # 🎯 Feature groups
    ROOMS      = ["bathroom1","bedroom1","bedroom2","livingroom1","garage1","kitchen1","office1"]
    APPLIANCES = ["clotheswasher1","dishwasher1","kitchenapp1","kitchenapp2","microwave1","range1","refrigerator1","venthood1","oven1"]
    LIGHTING   = ["lights_plugs1","lights_plugs2","lights_plugs3"]
    WEATHER    = ["temp","dwpt","rhum","prcp","wdir","wspd","pres","coco"]
    ENERGY     = ["total_energy"]

    feature_groups = {
        "rooms":      [f for f in ROOMS if f in avail],
        "appliances": [f for f in APPLIANCES if f in avail],
        "lighting":   [f for f in LIGHTING if f in avail],
        "weather":    WEATHER,
        "energy":     ENERGY,
    }

    print("🏠 House rows:", len(merged_df))
    print("📅 Date range:", merged_df.date.min(), "→", merged_df.date.max())
    print("🛏️ Rooms:", feature_groups["rooms"])
    print("🔌 Appliances:", feature_groups["appliances"])
    print("💡 Lighting:", feature_groups["lighting"])

    # ✅ Cell 2 – Aggregate Yesterday, Today & 7-Day Averages, plus Time-Buckets

    # Define time buckets based on usage patterns
    BUCKETS = {
        "morning":       list(range(6, 10)),      # 6–9
        "depart_work":   list(range(10, 14)),     # 10–13
        "return_work":   list(range(14, 17)),     # 14–16
        "evening":       list(range(17, 21)),     # 17–20
        "night":         list(range(21, 24)) + list(range(0, 6)),
    }


    def bucket_averages(data, features):
        out = {}
        for name, hours in BUCKETS.items():
            sub = data[data.hour.isin(hours)]
            out[name] = {f: round(float(sub[f].mean()), 5)
                        for f in features if f in sub.columns}
        return out


    # Data slices
    y_df = merged_df[merged_df.date == yesterday]
    t_df = merged_df[merged_df.date == report_date]
    h7 = merged_df[(merged_df.date >= yesterday - timedelta(days=7))
                & (merged_df.date < yesterday)]

    # 7-day hourly mean values
    h7_hourly = (
        h7.set_index("local_15min")
        [ENERGY + feature_groups["rooms"] + feature_groups["appliances"] +
            feature_groups["lighting"] + WEATHER]
        .resample("H").mean()
    )


    def get_season(date):
        """Return the season name (Northern Hemisphere logic)."""
        month = date.month
        day = date.day
        if (month == 12 and day >= 21) or (1 <= month <= 2) or (month == 3 and day < 20):
            return "Winter"
        elif (month == 3 and day >= 20) or (4 <= month <= 5) or (month == 6 and day < 21):
            return "Spring"
        elif (month == 6 and day >= 21) or (7 <= month <= 8) or (month == 9 and day < 22):
            return "Summer"
        else:
            return "Autumn"


    def summarize_day(data, label):
        summary = {
            "label": label,
            "total_energy": round(float(data.total_energy.sum()), 3),
            "peak_hours": list(data.groupby("hour").total_energy.sum().nlargest(3).index),
            "breakdown": {
                "rooms": round(data[feature_groups["rooms"]].sum().sum(), 3),
                "appliances": round(data[feature_groups["appliances"]].sum().sum(), 3),
                "lighting": round(data[feature_groups["lighting"]].sum().sum(), 3),
            },
            "weather": {
                "min": round(data.temp.min(), 2),
                "mean": round(data.temp.mean(), 2),
                "max": round(data.temp.max(), 2),
                "desc": WEATHER_MAP.get(int(data.coco.mode().iloc[0]), "Unknown"),
            },
            "season": get_season(data.date.iloc[0]),
            "buckets": bucket_averages(data, ["total_energy"] + feature_groups["rooms"] + feature_groups["appliances"]),
            "7d_avg": {k: round(float(v), 3) for k, v in h7_hourly.mean().items() if k in data.columns},
        }
        return summary


    # Weather condition code lookup
    WEATHER_MAP = {
        1: "Clear", 2: "Fair", 3: "Cloudy", 4: "Overcast", 5: "Fog", 6: "Freezing Fog",
        7: "Light Rain", 8: "Rain", 9: "Heavy Rain", 10: "Freezing Rain", 11: "Heavy Freezing Rain",
        12: "Sleet", 13: "Heavy Sleet", 14: "Light Snowfall", 15: "Snowfall", 16: "Heavy Snowfall",
        17: "Rain Shower", 18: "Heavy Rain Shower", 19: "Sleet Shower", 20: "Heavy Sleet Shower",
        21: "Snow Shower", 22: "Heavy Snow Shower", 23: "Lightning", 24: "Hail", 25: "Thunderstorm",
        26: "Heavy Thunderstorm", 27: "Storm"
    }

    # 🧠 Run the summaries
    y_stats = summarize_day(y_df, label="yesterday")
    t_stats = summarize_day(t_df, label="today")

    # Cell 3 – Build JSON Context for LLM

    def clean(d):
        if isinstance(d, dict):
            return {k: clean(v) for k,v in d.items()}
        if isinstance(d, list):
            return [clean(v) for v in d]
        if isinstance(d, (np.integer,np.int64)): return int(d)
        if isinstance(d, (np.floating,np.float64)): return float(d)
        return d


    context = {
        "house_id": HOUSE_ID,
        "report_date": REPORT_DAY,
        "yesterday": summarize_day(y_df, "Yesterday"),
        "today": summarize_day(t_df, "Today")
    }
    with open("llm_ctx.json", "w") as f:
        json.dump(context, f, indent=2)

    # Cell 4 – Build Instruction + Seasonal Few-Shot Prompt for LLM

    # 🧠 Instruction always included
    instruction = """
    You are a residential energy assistant.

    Your job is to generate a helpful, structured report based on energy usage JSON input. Focus on clear 4-part formatting with 5 bullet points per section.
    """.strip()

    # 🌦️ Few-shot examples by season
    few_shot_examples = {
        "Winter": """
    ---
    Example output (Winter):

    1) Analysis -> [
    - Total energy yesterday: 92.4 kWh, 6% above 7-day avg.
    - Peak: 19h (24.2 kWh) driven by oven1 and heating.
    - Morning bucket: 18.1 kWh (12% above norm).
    - Bedroom1 +40% of weekly mean.
    - Cloudy 3°C likely increased heating use.]

    2) Recommendations -> [
    - 3°C and overcast — lower thermostat, wear warm layers.
    - Preheat rooms early to avoid 19h surge.
    - Batch oven tasks after 20h.
    - Open curtains 8–16h for daylight.
    - Predicted: 95 kWh — target <90 kWh.]

    3) Alerts -> [
    - Oven1 ran 2h straight (+250% vs avg) — review usage.
    - Bedroom1 heating +40% above norm — check insulation.
    - Microwave1 spike at 13h (3.2 kWh) — investigate.
    - Dishwasher1 idle all day — unplug if unused.
    - Bill may increase ~11% if trend continues.]

    4) Tips -> [
    - Clean HVAC filters for winter efficiency.
    - Seal windows to stop heat loss.
    - Wrap water heater in insulation.
    - Use thermostat economy mode at night.
    - Book HVAC service before deeper cold sets in.]
    """,
        "Summer": """
    ---
    Example output (Summer):

    1) Analysis -> [
    - Total energy yesterday: 120.6 kWh, 2% above 7-day avg.
    - Peak: 11h (16.5 kWh) driven by kitchen1 and oven1.
    - Morning bucket: 4.4 kWh (38% above norm).
    - Kitchen1 +63% of weekly mean.
    - Evening usage: 6.3 kWh, up 18% from average.]

    2) Recommendations -> [
    - 31°C and clear — use fans instead of AC mid-day.
    - Avoid oven/range from 11h–16h.
    - Batch-cook meals to reduce appliance starts.
    - Use natural daylight until 20h.
    - Predicted: 120.6 kWh — aim for <115 kWh.]

    3) Alerts -> [
    - Kitchen1 +118% above norm — consider adjusting meal prep times.
    - Microwave1 spike at 13h — verify usage.
    - Dishwasher1 inactive — unplug to prevent standby draw.
    - Refrigerator1 ran longer than usual — check door seals.
    - Continued usage at this level may raise next week’s bill 15%.]

    4) Tips -> [
    - Clean ceiling fan blades to improve airflow.
    - Service AC before next heatwave.
    - Use blackout curtains to reduce cooling load.
    - Set fridge to 4°C and clean rear coils.
    - Run laundry at 21h to avoid peak charges.]
    """,
        "Spring": """
    ---
    Example output (Spring):

    1) Analysis -> [
    - Total usage: 88.2 kWh, 8% below 7-day average.
    - Peak: 20h (12.5 kWh), mostly from lighting and dishwasher.
    - Stable morning bucket at 15.4 kWh.
    - Bedroom1 and kitchen1 used moderate energy.
    - Weather was mild and partly cloudy.]

    2) Recommendations -> [
    - 19°C and sunny — open windows, reduce AC usage.
    - Shift cooking to midday for lower peak loads.
    - Use natural light from 8h–18h to save lighting.
    - Air out rooms instead of using HVAC.
    - Target 85 kWh today to stay below average.]

    3) Alerts -> [
    - Dishwasher1 +170% usage — may be misconfigured.
    - Microwave1 inactive recently — inspect for issues.
    - Kitchenapp2 spike at 11h — check automation schedule.
    - No significant lighting usage recorded.
    - If patterns persist, bill may increase by 6%.]

    4) Tips -> [
    - Clean refrigerator coils for spring efficiency.
    - Test CO/smoke detectors after winter.
    - Clean oven door seals monthly.
    - Prepare AC filters before summer starts.
    - Inspect windows for pollen-blocking seals.]
    """,
        "Autumn": """
    ---
    Example output (Autumn):

    1) Analysis -> [
    - Total usage: 97.5 kWh, right at 7-day average.
    - Peak: 18h due to oven1 and lighting circuits.
    - Cooler nights led to increased bedroom heating.
    - Kitchen1 remained the top consumer.
    - Weather ranged from 8°C to 15°C, partly cloudy.]

    2) Recommendations -> [
    - 12°C and cloudy — reduce thermostat by 1°C.
    - Limit oven1 and range1 use between 17h–19h.
    - Shift laundry to 10h or 21h off-peak slots.
    - Leverage early daylight to reduce lighting use.
    - Predicted usage: 99 kWh — aim for <95 kWh.]

    3) Alerts -> [
    - Bedroom2 heating +35% over 7d average — check insulation.
    - Microwave1 had a sharp 1.8 kWh spike at 19h.
    - Dishwasher1 skipped cycles — inspect for clogs.
    - Range1 ran idle at 13h — review cooking patterns.
    - Projected bill could rise by 7% if not optimized.]

    4) Tips -> [
    - Clear leaves from HVAC vents.
    - Clean windows to improve passive heating.
    - Seal minor wall cracks for heat retention.
    - Schedule boiler check-up before winter.
    - Replace autumn-degraded door seals.]
    """
    }

    # 📥 Load season from context
    with open("llm_ctx.json", "r", encoding="utf-8") as f:
        context = json.load(f)
    season = context["today"]["season"]

    # 🧾 Compose final prompt
    instruction_fewshot_prompt = instruction + \
        "\n" + few_shot_examples.get(season, "")

    # 💾 Save to file
    with open("llm_instruction_fewshot_prompt.txt", "w", encoding="utf-8") as f:
        f.write(instruction_fewshot_prompt.strip())

    print(
        f"✅ Instruction + {season} example saved to llm_instruction_fewshot_prompt.txt")

    # ✅ Cell 5 – Generate Final Daily Energy Report Using gemma3:4b

    # 📥 Load context and prompt from saved files
    with open("llm_ctx.json") as f:
        context = json.load(f)

    with open("llm_instruction_fewshot_prompt.txt") as f:
        instruction_prompt = f.read()

    # 🧠 Final prompt: instruction + context
    final_prompt = (
        instruction_prompt.strip() +
        "\n\n---\n\nContext:\n" +
        json.dumps(context, indent=2) +
        "\n\nNow generate the 4-part energy report:"
    )

    load_dotenv()
    OLLAMA_HOST_IP = os.getenv('MY_IP')

    # Construct the full host URL
    ollama_host = f"http://{OLLAMA_HOST_IP}:11434"
    # 💬 Initialize Ollama client and call IREMS_reporter local model
    client = ollama.Client(host=ollama_host)

    response = client.generate(
        model="energy_reporter2",
        prompt=final_prompt,
        stream=False,
    )

    # 📝 Decode and save result
    generated_report = response['response'].strip()

    with open("llm_daily_report.txt", "w", encoding="utf-8") as f:
        f.write(generated_report)

    # 📊 Display preview or full report
    print("\n✅ Final Daily Energy Report:\n" + "-"*60)
    print(generated_report[:2000])  # Print first 2000 chars
    if len(generated_report) > 2000:
        print("📁 Full report saved to 'llm_daily_report.txt'")
    print("-"*60)