#### To start server for testing
`./manage.py runserver`

#### To start server using gunicorn
`gunicorn --bind 127.0.0.1:5000 manage:app`

#### Creating schema
```python
(web_monitor) $./manage.py shell
>>> db.create_all()
```
