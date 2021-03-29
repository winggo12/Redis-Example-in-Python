import json
import redis
r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

#Processing Data of Json File , where the sorted set is called popularItem:
#Algorithm DataProcessor()
def DataProcessor(orders_json='orders.json', products_json='products.json'):
    with open(orders_json) as data_file:
        orders = json.load(data_file)

    with open(products_json, encoding="utf8")as data_file:
        products = json.load(data_file)

    for order in orders:
        for product in order["products"]:
            ID = product["productId"]
            Quantity = product["quantity"]
            Title = products[ID-101]["title"]
            r.zincrby("popular:ItemID", ID, Quantity)
            r.zincrby("popular:Title", Title, Quantity)
    return 0

##Getting the top m most popular items by ID or Title
def GetTopM(sorted_list=None, m = 5):
    return r.zrevrange(sorted_list, 0 , m, withscores=False)

if __name__ == '__main__':
    DataProcessor()
    print( GetTopM("popular:ItemID", m=5) )
    print( GetTopM("popular:Title", m=5) )
