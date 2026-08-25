# Supported platforms

QuickRows targets the operating systems exercised by the workspace and packaging workflows.

| Platform | Supported baseline | Required release coverage |
| --- | --- | --- |
| macOS | Current and previous two major releases | Oldest supported and current macOS, signed/notarized DMG |
| Windows | Windows 10 22H2 and Windows 11 | Install, upgrade, file association, Unicode paths, signing |
| Linux | Current Ubuntu LTS and compatible modern desktop distributions | X11 and Wayland; `.deb` and AppImage when published |

Support means the native application can install, launch, open and save CSV files, receive file-open events, and complete the large-file validation pass. Older systems may work but are not release-blocking unless explicitly added to this matrix.

Update this document before changing a release support floor.
