# Use the latest secure version of R
FROM rocker/r-ver:4.4.2

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    libxml2-dev \
    unixodbc-dev \
    libsqlite3-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    pandoc \
    libfontconfig1-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff-dev \
    libjpeg-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfontconfig1 \
    libcairo2-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Setup Python environment
COPY requirements.txt .
RUN pip3 install --no-cache-dir --ignore-installed -r requirements.txt --break-system-packages

# 3. Setup R environment
COPY install_packages.R .
RUN Rscript install_packages.R

# 4. Set up the working directory
WORKDIR /app
COPY . /app

CMD ["sh", "-c", "python3 main_pipeline.py && python3 main_parser.py && Rscript analysis.R"]