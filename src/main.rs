#![allow(warnings)]

use not_download_manager::ui::{IdmApp, NotDownloadManagerApp};
use std::env;
use std::path::PathBuf;

fn main() -> eframe::Result<()> {
    #[cfg(target_os = "windows")]
    unsafe {
        if std::env::var("WGPU_BACKEND").is_err() {
            std::env::set_var("WGPU_BACKEND", "dx12");
        }
    }

    let args: Vec<String> = env::args().collect();
    let mut screenshot_path: Option<PathBuf> = None;
    let mut show_columns = false;
    let mut show_scheduler = false;
    let mut show_sort = false;

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--screenshot" || args[i] == "-s" {
            if i + 1 < args.len() {
                screenshot_path = Some(PathBuf::from(&args[i + 1]));
                i += 1;
            }
        } else if args[i] == "--columns-preview" {
            show_columns = true;
        } else if args[i] == "--scheduler-preview" {
            show_scheduler = true;
        } else if args[i] == "--sort-preview" {
            show_sort = true;
        }
        i += 1;
    }

    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title("Not Download Manager")
            .with_inner_size([1040.0, 660.0])
            .with_min_inner_size([760.0, 480.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Not Download Manager",
        native_options,
        Box::new(move |cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            let mut app = IdmApp::new(screenshot_path);
            if show_columns {
                app.temp_columns = app.columns.clone();
                app.selected_column_idx = Some(0);
                app.column_width_input = format!("{:.0}", app.columns[0].width);
                app.show_columns_modal = true;
            }
            if show_scheduler {
                app.show_scheduler_modal = true;
                app.scheduler_tab = 1;
                app.scheduler_selected_id = app.downloads.iter().find(|d| d.queue.is_some()).map(|d| d.id);
            }
            if show_sort {
                app.sort_column = Some("filename".to_string());
                app.sort_ascending = true;
            }
            Ok(Box::new(app))
        }),
    )
}
