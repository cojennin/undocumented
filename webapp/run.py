from templates import app
#Load this config object for development mode
app.config.from_object('configurations.DevelopmentConfig')
app.secret_key = b'sjdjjdwqqeejdioqqjdioqdjqoidjd'
app.run()