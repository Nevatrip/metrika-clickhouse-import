#/bin/sh

PASSWORD=$(base64 -w 0 < /dev/urandom | head -c 32); echo $PASSWORD; echo $PASSWORD | sha256sum | tr -d '* -'


