#!/bin/sh

NETWORK_NAME="networka"
HOSTS="500"
OBSERVERS="4"
KEYS="6"

ruby load_inducer.rb ${NETWORK_NAME} ${HOSTS} ${OBSERVERS} ${KEYS}
