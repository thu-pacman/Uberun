from SSconfig import SSConfig as CFG
'''
Logger is used for better output or notification
'''
class SSLogger:
    def __init__(self, name, colorful=CFG.LOG['colorful'], 
                 error=CFG.LOG['error'], warn=CFG.LOG['warn'],
                 debug=CFG.LOG['debug'], info=CFG.LOG['info'], 
                 succ=CFG.LOG['succ'], echo=CFG.LOG['echo']):
        self.name = name
        self.FLAG_ERROR = error 
        self.FLAG_WARN = warn 
        self.FLAG_DEBUG = debug 
        self.FLAG_INFO = info
        self.FLAG_SUCC = succ
        self.FLAG_ECHO = echo
        # disable color
        self.red = "\x1b[31m" if colorful else ''
        self.green = "\x1b[32m" if colorful else ''
        self.yellow = "\x1b[33m" if colorful else ''
        self.blue = "\x1b[34m" if colorful else ''
        self.magenta = "\x1b[35m" if colorful else ''
        self.cyan = "\x1b[36m" if colorful else ''
        self.reset = "\x1b[0m" if colorful else ''


    # to print error in red
    def error(self, *args, **kwargs):
        if not self.FLAG_ERROR: 
            return
        msg = self.name + ' >>> ' + ' '.join(map(str, args))
        print(self.red + msg + self.reset, **kwargs, flush=True)
    
    def warn(self, *args, **kwargs):
        if not self.FLAG_WARN: 
            return
        msg = self.name + ' >>> ' + ' '.join(map(str, args))
        print(self.yellow + msg + self.reset, **kwargs, flush=True)

    def debug(self, *args, **kwargs):
        if not self.FLAG_DEBUG: 
            return
        msg = self.name + ' >>> ' + ' '.join(map(str, args))
        print(self.magenta + msg + self.reset, **kwargs, flush=True)

    def info(self, *args, **kwargs):
        if not self.FLAG_INFO: 
            return
        msg = self.name + ' >>> ' + ' '.join(map(str, args))
        print(self.blue + msg + self.reset, **kwargs, flush=True)

    def succ(self, *args, **kwargs):
        if not self.FLAG_SUCC: 
            return
        msg = self.name + ' >>> ' + ' '.join(map(str, args))
        print(self.green + msg + self.reset, **kwargs, flush=True)

    def echo(self, *args, **kwargs):
        if not self.FLAG_ECHO: 
            return
        msg = self.name + ' >>> ' + ' '.join(map(str, args))
        print(msg, **kwargs, flush=True)
