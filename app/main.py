import yaml
from aggregator import fetch_history, process_grid_power, aggregate
from tariffs import FlatRate, Economy7
from simulator import simulate


def load_config():
    with open("../config/settings.yaml") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()
    api_url = config["mqtt"]["api_url"]

    topic = "total/grid_power/state"

    print("Fetching data...")
    df = fetch_history(api_url, topic, "7d")

    print("Processing...")
    df = process_grid_power(df)
    df = aggregate(df)

    tariffs = [FlatRate(), Economy7()]

    print("\nResults (last 7 days):\n")
    for t in tariffs:
        cost = simulate(df, t)
        print(f"{t.name}: £{cost}")


if __name__ == "__main__":
    main()