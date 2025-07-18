import xml.etree
import xml.etree.ElementTree
import requests
import xml

BASE_URL = "https://www.nrl.com"

s = requests.Session()

def parse_sitemap():
    r = s.get(f"{BASE_URL}/sitemap/sitemap.xml",  headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0"})
    print(r.headers)
    r.raise_for_status()

    tree = xml.etree.ElementTree.fromstring(r.content)
    for node in tree.iter():
        print(node.attrib)


if __name__ == "__main__":
    print(parse_sitemap())