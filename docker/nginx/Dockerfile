FROM node:13.12.0-alpine as build

WORKDIR /stockosaurus-app/app
COPY ./stockosaurus-app/app/package.json .
COPY ./stockosaurus-app/app/package-lock.json .
RUN npm ci --silent
COPY /stockosaurus-app .
RUN npm run build


FROM nginx:stable-alpine
COPY --from=build /app/frontend/build /usr/share/nginx/html
CMD ["nginx", "-g", "daemon off;"]