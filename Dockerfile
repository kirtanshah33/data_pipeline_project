# FROM python
FROM apache/spark-py
WORKDIR /opt/application/

# RUN python -m pip install --upgrade pip && pip3 install pyspark==3.3.0
USER root
RUN apt-get update -y &&\
    apt-get install -y python3 &&\
    pip3 install pyspark==3.3.0

# ENTRYPOINT [ "/opt/scripts/spark/entrypoint.sh" ]
COPY requirements.txt /opt/application/
RUN pip3 install -r requirements.txt
COPY . /opt/application/
CMD [ "bash" ]
#CMD ["python3", "test.py"]
# FROM apache/spark-py

# ENV PYSPARK_MAJOR_PYTHON_VERSION=3
# WORKDIR /opt/application/

# # COPY requirements.txt .

# # RUN pip3 install -r requirements.txt
# COPY ./main.py .

# # COPY main.py .