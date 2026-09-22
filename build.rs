//! 构建脚本：编译 Slint 界面定义（`ui/scheduler.slint`）。
//!
//! 生成的代码通过 `slint::include_modules!()` 在 `main.rs` 中引入。

fn main() {
    slint_build::compile("ui/scheduler.slint").expect("Slint 界面编译失败");
}
