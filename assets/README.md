# Application artwork

- `app-icon.png` is the high-resolution source artwork.
- `icons/` contains generated platform packaging assets.

The packaging configuration consumes `32x32.png`, `128x128.png`, `128x128@2x.png`, `icon.icns`, and `icon.ico`. Regenerate all outputs from the same square source image and inspect them at native size before replacing tracked assets. Preserve transparency and avoid changing the package identifier or icon filenames without updating `packager.toml`.

A future automated icon pipeline should produce these files deterministically; until then, record the image tool and export settings in the pull request that updates them.
