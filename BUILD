cc_library(
	name = "sofa_transporter",
	srcs = [
            'core/transporter/sofarpc.cc',
            'core/transporter/service.cc',
	],
	deps = [
            '//thirdparty/sofa:sofa_rpc'
	]
)

cc_library(
    name = 'raft',
    srcs = [
        'core/server.cc',
        'core/peer.cc',
        'core/log.cc',
        'core/persist.cc',
    ],
    deps = [
        '//raft/base:base',
        '//thirdparty/muduo:muduo_base',
        '//toft/base:random',
        '//toft/system/atomic:atomic',
        ':raft_proto',
        '//thirdparty/glog:glog',
    ]
)

proto_library(
    name = 'raft_proto',
    srcs = ['core/raft.proto']
)

cc_test(
    name = "raft_test",
    srcs = [
        "core/persist_test.cc"
    ],
    deps = [
        ":raft",
    ]
)
