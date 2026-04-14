# Base image: A version of Linux with R and Shiny pre-installed
FROM --platform=linux/amd64 rocker/shiny-verse:latest

# Install Linux system libraries required for R packages
# (RPostgres needs libpq, Tidyverse needs xml2/ssl, etc.)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    libssl-dev \
    libxml2-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*


RUN R -e "install.packages(c('shinydashboard', 'tidyverse', 'lubridate', 'plotly', 'DBI', 'RPostgres', 'scales', 'DT', 'shinyWidgets'), repos='https://cloud.r-project.org/')"

# Copy  app code into the container
COPY app.R /srv/shiny-server/

# Expose the port used by Shiny
EXPOSE 3838

# Run the app
CMD ["R", "-e", "shiny::runApp('/srv/shiny-server/app.R', host = '0.0.0.0', port = 3838)"]