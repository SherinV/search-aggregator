// Copyright (c) 2021 Red Hat, Inc.
// Copyright Contributors to the Open Cluster Management project
module github.com/open-cluster-management/search-aggregator

go 1.16

require (
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/google/go-cmp v0.5.4 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gorilla/mux v1.7.4
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jlpadilla/benchmark-postgres v0.0.0-20210902155504-65a47037ff2f
	github.com/json-iterator/go v1.1.9 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/olekukonko/tablewriter v0.0.4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/redislabs/redisgraph-go v2.0.2+incompatible
	github.com/stretchr/testify v1.7.0
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/api v0.20.0 // indirect
	k8s.io/apimachinery v0.20.0 // indirect
	k8s.io/client-go v13.0.0+incompatible
	k8s.io/utils v0.0.0-20200327001022-6496210b90e8 // indirect
)

replace (
	github.com/buger/jsonparser => github.com/buger/jsonparser v1.1.1
	github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.1.0
	github.com/docker/docker => github.com/docker/docker v1.13.1
	github.com/open-cluster-management/api => github.com/open-cluster-management/api v0.0.0-20200623215229-19a96fed707a
	github.com/terraform-providers/terraform-provider-azurerm => github.com/terraform-providers/terraform-provider-azurerm v0.0.0-20200604143437-d38893bc4f78
	k8s.io/api => k8s.io/api v0.17.4
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.17.4
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.7
	k8s.io/apiserver => k8s.io/apiserver v0.17.4
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.17.4
	k8s.io/client-go => k8s.io/client-go v0.17.4
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.17.4
	k8s.io/cloud-provides => k8s.io/cloud-provides v0.17.4
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.17.4
	k8s.io/code-generator => k8s.io/code-generator v0.17.4
	k8s.io/component-base => k8s.io/component-base v0.17.4
	k8s.io/cri-api => k8s.io/cri-api v0.17.4
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.17.4
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.17.4
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.17.4
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.17.4
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.17.4
	k8s.io/kubectl => k8s.io/kubectl v0.17.4
	k8s.io/kubelet => k8s.io/kubelet v0.17.4
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.17.4
	k8s.io/metrics => k8s.io/metrics v0.17.4
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.18.1
	sigs.k8s.io/cluster-api-provider-aws => sigs.k8s.io/cluster-api-provider-aws v0.4.0
	sigs.k8s.io/cluster-api-provider-azure => sigs.k8s.io/cluster-api-provider-azure v0.4.0
	sigs.k8s.io/cluster-api-provider-openstack => sigs.k8s.io/cluster-api-provider-openstack v0.3.0
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.5.0
	sigs.k8s.io/yaml => sigs.k8s.io/yaml v1.2.0 // indirect
)
