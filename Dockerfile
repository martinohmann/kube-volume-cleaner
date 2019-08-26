FROM scratch

COPY kube-volume-cleaner /kube-volume-cleaner

ENTRYPOINT ["/kube-volume-cleaner"]
