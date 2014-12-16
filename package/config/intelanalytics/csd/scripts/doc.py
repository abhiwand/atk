import tornado.ioloop
import tornado.web
import argparse

parser = argparse.ArgumentParser(description="Get CDH configurations for ATK")
parser.add_argument("--host", type=str, help="Documentation Server Host address", default="0.0.0.0")
parser.add_argument("--port", type=int, help="Documentation Server Port ", default=80)
parser.add_argument("--path", type=str, help="Documentation path", default="admin")

args = parser.parse_args()

settings = {'debug': True,
            'static_path': args.path}

application = tornado.web.Application([
    (r"/", tornado.web.StaticFileHandler),
    ],**settings)

if __name__ == "__main__":
    application.listen(args.port, args.host)
    tornado.ioloop.IOLoop.instance().start()

