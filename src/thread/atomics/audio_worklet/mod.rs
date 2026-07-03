//! Audio worklet extension implementations.

mod js;
#[cfg(feature = "message")]
pub(super) mod main;
mod processor;
pub(super) mod register;

use std::any::{Any, TypeId};
use std::ptr::NonNull;

use js_sys::{JsString, Object, Reflect};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsCast;
use web_sys::{AudioWorkletNode, AudioWorkletNodeOptions, BaseAudioContext};

use self::js::{BaseAudioContextExt, ProcessorOptions};
pub(in super::super) use self::processor::register_processor;
#[cfg(feature = "message")]
pub(in super::super) use self::register::message::register_thread_with_message;
pub(in super::super) use self::register::{
	register_thread, AudioWorkletHandle, RegisterThreadFuture,
};
pub(in super::super) use super::is_main_thread;
use crate::web::audio_worklet::{AudioWorkletNodeError, ExtendAudioWorkletProcessor};

#[wasm_bindgen]
#[rustfmt::skip]
extern "C" {
	/// Name of our custom property on [`AudioWorkletNodeOptions`].
	#[wasm_bindgen(thread_local, static_string)]
	static DATA_PROPERTY_NAME: JsString = "__web_thread_data";

	/// Name of the
	/// [`AudioWorkletNodeOptions.processorOptions`](https://developer.mozilla.org/en-US/docs/Web/API/AudioWorkletNode/AudioWorkletNode#processoroptions)
	/// property.
	#[wasm_bindgen(thread_local, static_string)]
	static PROCESSOR_OPTIONS_PROPERTY_NAME: JsString = "processorOptions";
}

/// Returns [`true`] if this context has a registered thread.
pub(in super::super) fn is_registered(context: &BaseAudioContext) -> bool {
	matches!(
		context.unchecked_ref::<BaseAudioContextExt>().registered(),
		Some(true)
	)
}

/// Implementation for
/// [`crate::web::audio_worklet::BaseAudioContextExt::audio_worklet_node()`].
pub(in super::super) fn audio_worklet_node<P: 'static + ExtendAudioWorkletProcessor>(
	context: &BaseAudioContext,
	name: &str,
	data: P::Data,
	options: Option<&AudioWorkletNodeOptions>,
) -> Result<AudioWorkletNode, AudioWorkletNodeError<P>> {
	let options: &AudioWorkletNodeOptions = match options {
		Some(options) => options.unchecked_ref(),
		None => &Object::new().unchecked_into(),
	};

	let data = Box::new(Data {
		type_id: TypeId::of::<P>(),
		value: Box::new(data),
	});
	let data: NonNull<Data> = NonNull::from(Box::leak(data));

	// Store data directly on options (not via processorOptions) to work around
	// a Chrome bug where setting processorOptions causes AudioWorkletNode
	// name lookup to fail.
	DATA_PROPERTY_NAME.with(|name| {
		Reflect::set(options, name, &js_sys::Number::from(data.as_ptr() as u32))
			.expect("expected options to be an Object");
	});

	let result = AudioWorkletNode::new_with_options(context, name, options);

	// Clean up our custom property
	DATA_PROPERTY_NAME.with(|name| {
		Reflect::delete_property(options, name)
			.expect("expected options to be an Object");
	});

	match result {
		Ok(node) => Ok(node),
		Err(error) => Err(AudioWorkletNodeError {
			// SAFETY: We just made this pointer above and `new AudioWorkletNode` has to guarantee
			// that on error transmission failed to avoid double-free.
			data: *unsafe { Box::from_raw(data.as_ptr()) }
				.value
				.downcast()
				.expect("wrong type encoded"),
			error: super::error_from_exception(error),
		}),
	}
}

/// Data stored on [`AudioWorkletNodeOptions`] to transport
/// [`ExtendAudioWorkletProcessor::Data`].
struct Data {
	/// [`TypeId`] to compare to the type when arriving at the constructor.
	type_id: TypeId,
	/// [`ExtendAudioWorkletProcessor::Data`].
	value: Box<dyn Any>,
}
