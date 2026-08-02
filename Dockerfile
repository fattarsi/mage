# syntax=docker/dockerfile:1
#
# XMage Web Gateway — play vs AI in the browser.
# Build:  docker build -t xmage-web .
# Run:    docker run --rm -p 8080:8080 xmage-web
#         (optional deck source) docker run --rm -p 8080:8080 \
#             -e XMAGE_WEB_DECK_SOURCE_URL=https://your-deck-host xmage-web
# Then open http://localhost:8080/
#
# The build compiles the full XMage card pool, so the first build is heavy
# (many minutes, a few GB of RAM). Subsequent builds reuse Docker layer cache.

# ---- build stage: compile the gateway and everything it depends on ----
# Pin the exact Maven version (not the floating :3.9 tag) so the build is reproducible and doesn't
# depend on whatever :3.9 image the host's BuildKit happens to have cached — a stale/mismatched base
# was breaking the server build after an upstream bump to maven-compiler-plugin 3.15.0.
FROM maven:3.9.16-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
# -am builds the modules Mage.Server.Web needs (engine, sets, server, plugins).
RUN mvn -B -ntp -pl Mage.Server.Web -am install -DskipTests
# Pre-cache the exec plugin into ~/.m2 so the runtime's offline `mvn -o exec:java` can find it
# (the install above never invokes exec:, so it would otherwise be missing -> offline failure).
RUN mvn -B -ntp org.codehaus.mojo:exec-maven-plugin:3.1.0:help >/dev/null

# ---- runtime stage: run the built reactor offline via the exec plugin ----
# We keep Maven here because the gateway is launched through exec:java against the
# reactor classpath; the populated ~/.m2 from the build lets it run fully offline.
FROM maven:3.9.16-eclipse-temurin-17
WORKDIR /app
COPY --from=build /src /app
COPY --from=build /root/.m2 /root/.m2

EXPOSE 8080
# Optional external deck source (enables Commander + loading saved decks). Empty = off.
ENV XMAGE_WEB_DECK_SOURCE_URL=""

CMD ["mvn", "-o", "-ntp", "-pl", "Mage.Server.Web", "exec:java", \
     "-Dexec.mainClass=mage.server.web.WebServerMain", \
     "-Dxmage.web.port=8080", \
     "-Dxmage.config.path=Mage.Server/config/config.xml"]
