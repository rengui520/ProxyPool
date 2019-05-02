import time
from multiprocessing import Process
import asyncio
import aiohttp
try:
    from aiohttp.errors import ProxyConnectionError,ServerDisconnectedError,ClientResponseError,ClientConnectorError
except:
    from aiohttp import ClientProxyConnectionError as ProxyConnectionError,ServerDisconnectedError,ClientResponseError,ClientConnectorError
from proxypool.db import RedisClient
from proxypool.error import ResourceDepletionError
from proxypool.getter import FreeProxyGetter
from proxypool.setting import *
from asyncio import TimeoutError


class ValidityTester(object):
    test_api = TEST_API  #设置全局变量，检测代理网址

    def __init__(self):  #init定义基础变量
        self._raw_proxies = None
        self._usable_proxies = []

    def set_raw_proxies(self, proxies):  #set_raw_proxies将代理数组传过来
        self._raw_proxies = proxies  #设置成类变量
        self._conn = RedisClient()

    async def test_single_proxy(self, proxy):  #async实现异步检测。test_single_proxy
        """
        text one proxy, if valid, put them to usable_proxies.
        """
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    if isinstance(proxy, bytes):
                        proxy = proxy.decode('utf-8')
                    real_proxy = 'http://' + proxy
                    print('Testing', proxy)
                    async with session.get(self.test_api, proxy=real_proxy, timeout=get_proxy_timeout) as response:
                        if response.status == 200:
                            self._conn.put(proxy)  #put方法，把队列保存下来
                            print('Valid proxy', proxy)
                except (ProxyConnectionError, TimeoutError, ValueError):
                    print('Invalid proxy', proxy)
        except (ServerDisconnectedError, ClientResponseError,ClientConnectorError) as s:
            print(s)
            pass

    def test(self):
        """
        aio test all proxies.
        """
        print('ValidityTester is working')
        try:
            loop = asyncio.get_event_loop()
            tasks = [self.test_single_proxy(proxy) for proxy in self._raw_proxies]
            loop.run_until_complete(asyncio.wait(tasks))
        except ValueError:
            print('Async Error')


class PoolAdder(object):
    """
    add proxy to pool
    """

    def __init__(self, threshold):
        self._threshold = threshold
        self._conn = RedisClient()
        self._tester = ValidityTester()
        self._crawler = FreeProxyGetter()  #代理抓取的类

    def is_over_threshold(self):
        """
        judge if count is overflow.
        """
        if self._conn.queue_len >= self._threshold:
            return True
        else:
            return False

    def add_to_queue(self):
        print('PoolAdder is working')
        proxy_count = 0
        while not self.is_over_threshold():
            for callback_label in range(self._crawler.__CrawlFuncCount__):  #range遍历循环各个网站爬取函数
                callback = self._crawler.__CrawlFunc__[callback_label]  #依次从CrawlFunc拿出获取方法名称，
                raw_proxies = self._crawler.get_raw_proxies(callback)
                # test crawled proxies
                self._tester.set_raw_proxies(raw_proxies)
                self._tester.test()
                proxy_count += len(raw_proxies)
                if self.is_over_threshold():
                    print('IP is enough, waiting to be used')
                    break
            if proxy_count == 0:
                raise ResourceDepletionError


class Schedule(object):
    @staticmethod
    def valid_proxy(cycle=VALID_CHECK_CYCLE):  #传入了时间参数，定时检查的时间，
        """
        Get half of proxies which in redis
        """
        conn = RedisClient() #Redis连接对象
        tester = ValidityTester()  #检测代理是否可用
        while True:
            print('Refreshing ip')
            count = int(0.5 * conn.queue_len)  #拿出一半代理检测
            if count == 0:  #代理数量判断，
                print('Waiting for adding')
                time.sleep(cycle)  #数量不够时，进行休眠
                continue
            raw_proxies = conn.get(count)  #拿到代理之后，
            tester.set_raw_proxies(raw_proxies)  #设置为类变量
            tester.test()  #最后调用test方法检测，
            time.sleep(cycle)

    @staticmethod
    def check_pool(lower_threshold=POOL_LOWER_THRESHOLD,
                   upper_threshold=POOL_UPPER_THRESHOLD,
                   cycle=POOL_LEN_CHECK_CYCLE):  #定时检测，加一个时间的轮询间隔
        """
        If the number of proxies less than lower_threshold, add proxy
        """
        conn = RedisClient()
        adder = PoolAdder(upper_threshold)  #PoolAdder
        while True:  #判断数量
            if conn.queue_len < lower_threshold:
                adder.add_to_queue()  #add_to_queue
            time.sleep(cycle)

    def run(self):  #run方法重新运行了两个进程。一个为从网上获取代理，筛选并放到数据库中；一个为定时的从数据库拿出一些代理来进行检测。
        print('Ip processing running')
        valid_process = Process(target=Schedule.valid_proxy)  #target传入运行方法的名称。valid_proxy 定时检测器
        check_process = Process(target=Schedule.check_pool)   #check_pool，从各大网站获取代理，再检测代理是否可用，然后再将代理放到redis数据库中，
        valid_process.start()

        #定义两个process，多进程的库。调用start开启进程
        check_process.start()
