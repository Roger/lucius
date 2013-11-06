class FixCouchdbProxy(object):
    '''
    :param app: the WSGI application
    :param script_name ie. /_ftl/
    :param scheme ie. http
    '''
    def __init__(self, app, script_name="/_ftl/", scheme="http"):
        self.app = app
        self.script_name = "/_ftl/"
        self.scheme = scheme

    def __call__(self, environ, start_response):
        if self.script_name:
            environ['SCRIPT_NAME'] = self.script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(self.script_name):
                environ['PATH_INFO'] = path_info[len(self.script_name):]

        environ['wsgi.url_scheme'] = self.scheme
        return self.app(environ, start_response)

