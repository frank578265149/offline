#!/usr/bin/env bash
sudo iptables -t nat -A DOCKER ! -i br-c99c711d5d27 -p tcp -m tcp --dport 9999 -j DNAT --to-destination 172.16.11.3:9999

http://114.115.213.107:9123/proxy/application_1479432281477_0015/