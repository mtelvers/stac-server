# syntax=docker/dockerfile:1
FROM ocaml/opam:debian-13-ocaml-5.4 AS build
RUN sudo ln -sf /usr/bin/opam-2.4 /usr/bin/opam && opam init --reinit -ni
RUN sudo apt update && sudo apt-get --no-install-recommends install -y \
    ca-certificates lsb-release wget pkg-config m4 libev-dev libffi-dev libgmp-dev
RUN wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && sudo apt update \
    && sudo apt install -y -V libarrow-dev libparquet-dev
RUN cd ~/opam-repository && git fetch -q origin master && opam update
COPY --chown=opam --link stac_server.opam /src/stac_server.opam
WORKDIR /src
RUN echo '(lang dune 3.0)' | tee ./dune-project
RUN opam install -y --deps-only .
ADD --chown=opam . .
RUN opam exec -- dune build
# Collect shared library dependencies
RUN sudo mkdir -p /dist/lib && \
    ldd /src/_build/default/bin/server.exe | grep -oP '/\S+' | \
    xargs -I{} sudo cp -L {} /dist/lib/

FROM debian:13-slim
RUN apt update && apt-get --no-install-recommends install -y dumb-init \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build --link /dist/lib/ /usr/lib/
COPY --from=build --link /src/_build/default/bin/server.exe /usr/local/bin/stac-server
COPY --from=build --link /src/_build/default/bin/sync.exe /usr/local/bin/stac-sync
RUN ldconfig
ENTRYPOINT ["dumb-init"]
CMD ["/usr/local/bin/stac-server"]
