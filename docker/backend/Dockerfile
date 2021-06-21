FROM python:3.8.3-alpine

RUN mkdir /stockosaurus
COPY ./requirements.txt /stockosaurus/
WORKDIR /stockosaurus

RUN pip install --upgrade pip
RUN pip install gunicorn
RUN pip install -r requirements.txt

COPY . /stockosaurus/