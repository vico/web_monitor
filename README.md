#### To start server for testing
`./manage.py runserver`

#### To start server using gunicorn
`gunicorn --bind 127.0.0.1:5000 manage:app`

#### Creating schema
```python
from flaskr import init_db`
init_db()
```
