THRIFT_IMPORT_PATH := samsaradev.io/infra/dataplatform/samsara_thrift/thrift

gen:
	docker run -u $$(id -u):$$(id -g) -v "${PWD}:/data" thrift thrift -o /data --gen go:package_prefix=$(THRIFT_IMPORT_PATH)=,thrift_import=$(THRIFT_IMPORT_PATH) /data/TCLIService.thrift
	rm -r gen-go/tcliservice/t_c_l_i_service-remote

.phony: gen
