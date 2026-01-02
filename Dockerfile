FROM apache/spark:3.5.1

USER root

RUN pip install pandas xlrd

WORKDIR /app

COPY . . 

CMD ["bash"]
