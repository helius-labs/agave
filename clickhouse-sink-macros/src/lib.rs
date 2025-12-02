use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Measures function execution time and sends the result to ClickHouse.
///
/// # Usage
///
/// ```rust
/// #[measure_clickhouse]
/// fn my_function() {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn measure_clickhouse(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let fn_name_literal = input.sig.ident.to_string();

    let fn_vis = &input.vis;
    let fn_sig = &input.sig;
    let fn_block = &input.block;
    let fn_attrs = &input.attrs;

    let expanded = quote! {
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            let __measure_start = std::time::Instant::now();
            let __result = (|| #fn_block)();
            let __duration_us = __measure_start.elapsed().as_micros() as u64;

            let __timestamp_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;

            let __metadata = serde_json::json!({
                "fn_name": #fn_name_literal,
                "duration_us": __duration_us,
            });

            let __event = clickhouse_sink::tables::agave_events::AgaveEvent {
                event_type: "fn_latency".to_string(),
                slot: 0,
                timestamp_us: __timestamp_us,
                metadata: __metadata,
            };
            clickhouse_sink::sink::record(
                clickhouse_sink::table_batcher::TableRow::AgaveEvents(__event)
            );

            __result
        }
    };

    TokenStream::from(expanded)
}
