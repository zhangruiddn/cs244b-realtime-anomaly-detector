FROM rayproject/ray:2.9.0-py310

WORKDIR /usr/src/ai-alert

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
