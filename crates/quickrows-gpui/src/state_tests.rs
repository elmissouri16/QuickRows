//! Focused tests for the private workspace implementation.
//!
//! These child modules stay inside the crate so they can exercise the private
//! types and helpers shared by the concern-oriented `include!` files.

mod parsing_settings;
mod platform_runtime;
mod presentation;
mod selection;
mod state;
mod ui_interactions;
