import argparse

from src.ai_alert import AIAlert


def main():
    parser = argparse.ArgumentParser(description='Run AI Alert system.')
    parser.add_argument('--mode', type=str, default='ray', help='Execution mode (local or ray)')
    args = parser.parse_args()

    ai_alert = AIAlert('config.toml', mode=args.mode)
    ai_alert.run()


if __name__ == "__main__":
    main()
