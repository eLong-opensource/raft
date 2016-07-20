cc_library(
    name = 'base',
    srcs = [
        'address.cc',
        'kvconfig.cc',
        'selector.cc'
    ],
    deps = [
        '//thirdparty/muduo:muduo_base',
        '//thirdparty/glog:glog'
    ]
)

cc_test(
    name = "base_test",
    srcs = [
        "channel_test.cc",
        "selector_test.cc",
        "timer_test.cc"
    ],
    deps = ":base"
)
