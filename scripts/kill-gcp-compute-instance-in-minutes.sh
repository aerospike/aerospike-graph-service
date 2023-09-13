#!/usr/bin/env bash$0
set -e
sleep 9
echo "Killing ci-pe-${{ env.SHA_PATH }}-w-3"
gcloud compute instances delete ci-pe-${{ env.SHA_PATH }}-w-3 --zone=us-central1-a --quiet
echo "Killing ci-pe-${{ env.SHA_PATH }}-w-4"
gcloud compute instances delete ci-pe-${{ env.SHA_PATH }}-w-4 --zone=us-central1-a --quiet