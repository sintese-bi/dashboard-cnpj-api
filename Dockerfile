# start by pulling the python image
FROM python:3.9-alpine

# copy the requirements file into the image
COPY ./requirements/requirements.txt /app/requirements.txt

# switch working directory
WORKDIR /app

RUN \
    apk add --no-cache postgresql-libs && \
    apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
    python3 -m pip install -r requirements.txt --no-cache-dir && \
    apk --purge del .build-deps

# install the dependencies and packages in the requirements file
RUN pip install -r requirements.txt
RUN pip install pandas

# copy every content from the local file to the image
COPY . /app

EXPOSE 8080
# configure the container to run in an executed manner
ENTRYPOINT [ "python" ]
CMD ["api_cnpj.py" ]
