
Install nginx

```bash
sudo apt install nginx
sudo systemctl enable nginx
sudo systemctl start nginx
```

Assume the web app directory reside at /home/pi/webmonitor
We need to change permission of /home/pi

```bash
chmod 710 /home/pi
```

and add nginx to group of above user:

```bash
usermod -a -G pi www-data
```