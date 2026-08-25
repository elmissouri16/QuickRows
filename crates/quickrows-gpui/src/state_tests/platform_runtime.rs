//! Focused platform runtime tests.

use crate::{
    Modal, OpenTarget, QuickRowsView, RuntimeRequest, ShowShortcuts, file_fingerprint,
    open_target_from_value, path_from_open_value, requeue_deferred_runtime_requests,
};

#[gpui::test]
fn focused_workspace_dispatches_contextual_keyboard_shortcuts(cx: &mut gpui::TestAppContext) {
    use gpui::{AppContext as _, KeyBinding, Keystroke};

    let window = cx.update(|cx| {
        gpui_component::init(cx);
        cx.open_window(Default::default(), |window, cx| {
            let view = cx.new(|cx| QuickRowsView::new(None, window, cx));
            let weak_view = view.downgrade();
            view.update(cx, |view, _| view.runtime.self_weak = Some(weak_view));
            view
        })
        .unwrap()
    });
    cx.update(|cx| {
        cx.bind_keys([KeyBinding::new("ctrl-g", ShowShortcuts, Some("QuickRows"))]);
    });
    cx.dispatch_keystroke(*window, Keystroke::parse("ctrl-g").unwrap());
    window
        .update(cx, |view, _, _| {
            assert!(matches!(view.overlay.modal, Modal::Shortcuts))
        })
        .unwrap();
}

#[test]
fn deferred_runtime_request_preserves_the_remaining_batch() {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    let current = OpenTarget::from(std::path::PathBuf::from("first.csv"));
    let remaining_open = OpenTarget::from(std::path::PathBuf::from("second.csv"));
    let later_open = OpenTarget::from(std::path::PathBuf::from("third.csv"));
    let requests = Mutex::new(VecDeque::from([RuntimeRequest::Open(later_open)]));
    requeue_deferred_runtime_requests(
        &requests,
        RuntimeRequest::Open(current),
        VecDeque::from([
            RuntimeRequest::Activate,
            RuntimeRequest::Open(remaining_open),
        ]),
    );

    let mut requests = requests.into_inner().unwrap();
    assert!(
        matches!(requests.pop_front(), Some(RuntimeRequest::Open(target)) if target.path == std::path::Path::new("first.csv"))
    );
    assert!(matches!(
        requests.pop_front(),
        Some(RuntimeRequest::Activate)
    ));
    assert!(
        matches!(requests.pop_front(), Some(RuntimeRequest::Open(target)) if target.path == std::path::Path::new("second.csv"))
    );
    assert!(
        matches!(requests.pop_front(), Some(RuntimeRequest::Open(target)) if target.path == std::path::Path::new("third.csv"))
    );
    assert!(requests.is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_paths_survive_cli_and_instance_encoding() {
    use crate::{decode_open_target, encode_open_target, open_target_from_os_value};
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let mut name = b"rows-".to_vec();
    name.push(0xff);
    name.extend_from_slice(b".csv");
    let path = dir.path().join(std::ffi::OsString::from_vec(name));
    std::fs::write(&path, "a,b\n1,2\n").unwrap();

    let target = open_target_from_os_value(path.as_os_str()).unwrap();
    assert_eq!(target.path, path);
    let encoded = encode_open_target(&target);
    assert_eq!(decode_open_target(&encoded).unwrap().path, target.path);
}

#[test]
fn file_urls_preserve_unicode_paths() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("résumé-日本語.csv");
    std::fs::write(&path, "a,b\n1,2\n").unwrap();
    let url = url::Url::from_file_path(&path).unwrap();
    assert_eq!(path_from_open_value(url.as_str()), Some(path));
}

#[test]
fn file_url_fragments_are_percent_decoded_and_retained() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fragment.csv");
    std::fs::write(&path, "a,b\n1,2\n3,4\n").unwrap();
    let mut url = url::Url::from_file_path(&path).unwrap().to_string();
    url.push_str("#row%3D2-3");
    let target = open_target_from_value(&url).unwrap();
    assert_eq!(target.path, path);
    assert_eq!(target.fragment.unwrap().to_string(), "row=2-3");
}

#[test]
fn fingerprints_change_after_external_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("external.csv");
    std::fs::write(&path, "a\n1\n").unwrap();
    let before = file_fingerprint(&path).unwrap();
    std::fs::write(&path, "a\n123456\n").unwrap();
    let after = file_fingerprint(&path).unwrap();
    assert_ne!(before, after);
}
