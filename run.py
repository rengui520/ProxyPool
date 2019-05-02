from proxypool.api import app
from proxypool.schedule import Schedule

def main():
#运行调度器（为代理池定义的），和app.run
    s = Schedule()
    s.run()
    app.run()




if __name__ == '__main__':
    main()

