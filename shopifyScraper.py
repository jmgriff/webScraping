import requests, json, dataset, databases


class ShopifyScraper():

    def __init__(self, baseurl):
        self.baseurl = baseurl

    # https://www.allbirds.com/products.json <- Most shopify stores will format like this.
    def downloadjson(self, page):
        r = requests.get(self.baseurl + f'products.json?limit=250&page={page}', timeout=30)
        if r.status_code != 200:
            print('Bad Status Code: ', r.status_code)
        if len(r.json()['products']) > 0:
            data = r.json()['products']
            return data
        else:
            return

    def parsejson(self, jsondata):
        products = []
        for prod in jsondata:
            mainid = prod['id']
            title = prod['title']
            published_at = prod['published_at']
            product_type = prod['product_type']
            for v in prod['variants']:
                item = {
                    'id': mainid,
                    'title': title,
                    'published_at': published_at,
                    'product_type': product_type,
                    'varid': v['id'],
                    'vartitle': v['title'],
                    'sku': v['sku'],
                    'price': v['price'],
                    'available': v['available'],
                    'created_at': v['created_at'],
                    'compare_at_price': v['compare_at_price']
                }
                products.append(item)
        return products


def main():
    hstl = ShopifyScraper('https://us.hstlmade.com/')
    results = []
    for page in range(1, 10):
        data = hstl.downloadjson(page)
        print('Getting page: ', page)
        try:
            results.append(hstl.parsejson(data))
        except:
            print(f'Completed, total pages = {page - 1}')
            break
    return results


products = main()
totals = [item for i in products for item in i]
# print(products)
print('There were {} products'.format(len(totals)))

# Write json to local file

# def writeToJSONFile(path, fileName, data):
#     filePathNameWExt = './' + path + '/' + fileName + '.json'
#     with open(filePathNameWExt, 'w') as fp:

#         json.dump(data, fp)
# path = './.'
# fileName = 'pinkcherry'
# data = products
# writeToJSONFile('./','pinkcherry',data)


# creating a SQl lite database
if __name__ == '__main__':
    db = dataset.connect('sqlite:///products.db')
    table = db.create_table('hstlmade', primary_id='varid')
    products = main()
    totals = [item for i in products for item in i]

    for p in totals:
        if not table.find_one(varid=p['varid']):
            table.insert(p)
            # print('New Products: ', p)
        # Just for Testing
        else:
            print('skipping product')

