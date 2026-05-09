use clutchcall_sdk::method_id::*;

#[test]
fn method_id_constants_are_stable() {
    assert_eq!(METHOD_ID_ORIGINATE, 1430677891);
    assert_eq!(METHOD_ID_ORIGINATE_BULK, 721069100);
    assert_eq!(METHOD_ID_TERMINATE, 3834253405);
    assert_eq!(METHOD_ID_AUDIO_FRAME, 2991054320);
    assert_eq!(METHOD_ID_STREAM_EVENTS, 959835745);
}

#[test]
fn method_id_values_are_unique() {
    let ids = [
        METHOD_ID_ORIGINATE,
        METHOD_ID_ORIGINATE_BULK,
        METHOD_ID_ABORT_BULK,
        METHOD_ID_TERMINATE,
        METHOD_ID_STREAM_EVENTS,
        METHOD_ID_BARGE,
        METHOD_ID_AUDIO_FRAME,
    ];
    for (i, a) in ids.iter().enumerate() {
        for b in ids.iter().skip(i + 1) {
            assert_ne!(a, b, "duplicate method id: {}", a);
        }
    }
}
