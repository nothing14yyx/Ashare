"""项目启动脚本入口, 直接执行即可运行示例."""

from ashare.app import AshareApp


def main() -> None:
    AshareApp().run()


if __name__ == "__main__":
    main()
