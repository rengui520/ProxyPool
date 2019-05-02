import redis
from proxypool.error import PoolEmptyError
from proxypool.setting import HOST, PORT, PASSWORD


class RedisClient(object):
    def __init__(self, host=HOST, port=PORT):
        if PASSWORD:  #声明了几个redis的连接对象
            self._db = redis.Redis(host=host, port=port, password=PASSWORD)
        else:
            self._db = redis.Redis(host=host, port=port)

    #下边是几个方法
    def get(self, count=1):
        """
        get proxies from redis
        """
        proxies = self._db.lrange("proxies", 0, count - 1)  #lrange方法，从队列的左侧拿出多少个内容返回出来。从左侧批量获取的方法。
        self._db.ltrim("proxies", count, -1)
        return proxies

    def put(self, proxy):  #
        """
        add proxy to right top
        """
        self._db.rpush("proxies", proxy)  #rpush检测成功后，将代理放到右侧。

    def pop(self):  #pop方法供api使用
        """
        get proxy from right.
        """
        try:
            return self._db.rpop("proxies").decode('utf-8')  #rpop从右侧拿出一个可用代理
        except:
            raise PoolEmptyError

    @property
    def queue_len(self):  #队列长度
        """
        get length from queue.
        """
        return self._db.llen("proxies")

    def flush(self):
        """
        flush db
        """
        self._db.flushall()


if __name__ == '__main__':   #刷新整个代理队列
    conn = RedisClient()
    print(conn.pop())
