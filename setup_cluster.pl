#!/usr/bin/perl
use strict;
use warnings;

$| = 1;

print "Starting Vortex-Proxy Cluster Setup...\n";
print "-" x 50 . "\n";

sub run_cmd {
    my ($cmd, $ignore_fail) = @_;
    print "-> $cmd\n";
    my $exit_code = system($cmd);
    if ($exit_code != 0 && !$ignore_fail) {
        die "\nFATAL: Command failed with exit code " . ($exit_code >> 8) . ". Aborting.\n";
    }
}

print "\nCleaning up old cluster state...\n";
run_cmd("docker stop proxy kind-db backend-1 backend-2 >/dev/null 2>&1", 1);
run_cmd("docker rm proxy kind-db backend-1 backend-2 >/dev/null 2>&1", 1);
run_cmd("docker network rm vortex-net >/dev/null 2>&1", 1);

print "\nProvisioning Network & Backend Servers...\n";
run_cmd("docker network create vortex-net");
run_cmd("docker run -d --name backend-1 -p 8081:80 --network vortex-net nginxdemos/hello:plain-text");
run_cmd("docker run -d --name backend-2 -p 8082:80 --network vortex-net nginxdemos/hello:plain-text");

print "\nBooting Kind DB (Control Plane)...\n";
run_cmd("docker run -d --name kind-db -p 50051:50051 --network vortex-net nks01x/kind-db:latest");

print "\nBuilding and Booting Vortex-Proxy...\n";
run_cmd("docker build -t vortex-proxy:latest .");
run_cmd("docker run -d --name proxy --network vortex-net -p 8000:8000 -e KIND_DB_URL=\"http://kind-db:50051\" vortex-proxy:latest");

print "\nWaiting for gRPC Watch streams to establish (5 seconds)...\n";
sleep(5);

print "\nInjecting Test Route into Kind DB...\n";
my $grpcurl_cmd = q(grpcurl -plaintext -import-path ./proto -proto kind.proto -d '{"key": "router:testapp", "value": "eyJjbGllbnRfaWQiOiAidGVzdGFwcCIsICJpcHMiOiBbIjEyNy4wLjAuMTo4MDgxIiwgIjEyNy4wLjAuMTo4MDgyIl19"}' localhost:50051 kind.KindService/Put);
run_cmd($grpcurl_cmd, 1);

print "-" x 50 . "\n";
print "Cluster successfully deployed!\n\n";
print "Test the proxy routing by running:\n";
print "curl -i -H \"Host: testapp.vortex.cloud\" http://localhost:8000\n";