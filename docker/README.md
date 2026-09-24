# Docker

```bash
docker compose up --build -d
docker compose logs -f
docker compose logs -f ecommerce
```

- When trying to access the localhost we need to setup on the linux containers /etc/hosts

```bash
dajimenezriv@dajimenezriv:~$ cat /etc/hosts
127.0.0.1	localhost
127.0.1.1	dajimenezriv
```

By doing `extra_hosts: host.docker.internal:host-gateway` we make:

```bash
# /etc/hosts
# We get the hostIP by using the reserved part `host-gateway`.
hostIP host.docker.internal
```

We could do `docker run --rm --add-host host.docker.internal:host-gateway --add-host my-laptop:host-gateway alpine cat /etc/hosts` to add also the name `my-laptop`. With this command we are creaating an alpine container that we are going to remove as soon as the process exists and we add both hosts.
