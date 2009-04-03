Sam Tingleff - 12:04 PM 4/1/2009
	

1) Build and Install nginx + webdav
  apt-get install libpcre3-dev
  wget http://sysoev.ru/nginx/nginx-0.6.35.tar.gz
  tar xzf nginx-0.6.35.tar.gz
  cd nginx-0.6.35/
  ./configure --with-http_dav_module
  make
  sudo make install

2) Edit config
  edit nginx.conf (see nginx-sample.conf) and copy into /usr/local/nginx/conf/nginx.conf

3) Start nginx
  cd /usr/local/nginx
  sudo ./sbin/nginx

In nginx.conf make sure you set the port, the document root and client_body_temp_path.

client_body_temp_path should probably be on the same file system as document root.
