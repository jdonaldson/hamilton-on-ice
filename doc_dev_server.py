#!/usr/bin/env python
from livereload import Server, shell
import webbrowser
server = Server()
server.watch('docs/*.rst', shell('python setup.py docs'))
address = "http://127.0.0.1:5500"
webbrowser.open(address)
server.serve(root='build/sphinx/html')

