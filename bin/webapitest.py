# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import argparse
from mainapp.libs.signature import CreateSignature
import urllib2
import urllib
import sys
import os

parser = argparse.ArgumentParser(description="submit data to web api and print the return data")
parser.add_argument("method", type=str, choices=["get", "post"], help="get or post")
parser.add_argument("api", type=str, help="the api address and url")
parser.add_argument("-n", "--name", type=str, help="names for data")
parser.add_argument("-d", "--data", type=str, help="data or files for names")
parser.add_argument("-c", "--client", type=str, help="client name", default="test")
parser.add_argument("-b", "--base_url", type=str, help="the base url of api", default="http://192.168.10.126/app/")

args = parser.parse_args()

if args.data and not args.name:
    print("Error:must give the name option when the data is given!")
    sys.exit(1)

if args.name and not args.data:
    print("Error:must give the data option when the name is given!")
    sys.exit(1)


def main():
    datas = {}
    if args.name:
        names_list = args.name.split(",")
        data_list = args.data.split(",")
        for index in range(len(names_list)):
            if index >= len(data_list):
                if os.path.isfile(data_list[index]):
                    with open(data_list[index],"r") as f:
                        content = f.readall()
                else:
                    content = data_list[index]
                datas[names_list[index]] = content
            else:
                datas[names_list[index]] = ""
    httpHandler = urllib2.HTTPHandler(debuglevel=1)
    httpsHandler = urllib2.HTTPSHandler(debuglevel=1)
    opener = urllib2.build_opener(httpHandler, httpsHandler)

    urllib2.install_opener(opener)
    data = urllib.urlencode(datas)

    if args.method == "post":
        url = "%s%s" % (args.base_url, args.api)
        print("post data to url %s ...\n\n" % url)
        print("post data:\n%s" % data)
        request = urllib2.Request(url, data)
    else:
        if args.api.index("?"):
            url = "%s%s&%s" % (args.base_url, args.api, data)
        else:
            url = "%s%s?%s" % (args.base_url, args.api, data)
        print("request to url %s ..." % url)
        request = urllib2.Request(url)

    signature_obj = CreateSignature(args.client)
    request.add_header('client', signature_obj.client)
    request.add_header('nonce', signature_obj.nonce)
    request.add_header('timestamp', signature_obj.timestamp)
    request.add_header('signature', signature_obj.signature)

    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError, e:
        print("HTTP stat code:%s\n%s \n" % (e.code, e))
    else:
        the_page = response.read()
        print("Return page:\n%s" % the_page)

if __name__ == "__main__":
    main()
