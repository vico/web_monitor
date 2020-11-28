#### To start server for testing
`./manage.py runserver`

#### To start server using gunicorn
`gunicorn --bind 127.0.0.1:5000 manage:app`

#### Start test maria db
```bash
docker run --name mariadb -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -d   mariadb:latest
```

check for exposed ip

```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mariadb
```



#### Creating schema
```python
./manage.py shell
db.create_all()
```
