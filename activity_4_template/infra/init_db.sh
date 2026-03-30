aerich init -t app.database.TORTOISE_ORM
aerich init-db

## Remove database
#  docker volume ls | grep carlemany-backend-data | awk '{ print $2 }' | xargs docker volume rm