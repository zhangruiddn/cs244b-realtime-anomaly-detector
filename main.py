import argparse

from src.realtime_alert import RealtimeAlert


def main():
    parser = argparse.ArgumentParser(description='Run Realtime Alert system.')
    parser.add_argument('--mode', type=str, default='ray', help='Execution mode (local or ray)')
    args = parser.parse_args()

    ai_alert = RealtimeAlert('config.toml', mode=args.mode)
    ai_alert.run()


if __name__ == "__main__":
    main()
