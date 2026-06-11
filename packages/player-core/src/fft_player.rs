use spectrum_analyzer::*;
use tracing::error;

/// 一个接收音频 PCM 数据并转换成频谱的伪播放结构
/// 该结构会将传入的音频数据转换为单通道音频数据，然后进行频谱分析
pub struct FFTPlayer {
    sliding_window: [f32; 2048],
    result_buf: [f32; 2048],
    freq_range: (f32, f32),
    sample_rate: u32,
}

// numpy.interp()
#[allow(unused)]
fn vec_interp(src: &[f32], dst: &mut [f32]) {
    if src.is_empty() {
        dst.fill(0.0);
        return;
    }
    if dst.is_empty() {
        return;
    }
    if src.len() == dst.len() {
        dst.copy_from_slice(src);
        return;
    }
    let src_len = src.len();
    let dst_len = dst.len();
    let src_step = src_len as f32 / dst_len as f32;
    let mut src_idx = 0.0;
    for dst in dst.iter_mut() {
        let src_idx_int = src_idx as usize;
        let src_idx_frac = src_idx - src_idx_int as f32;
        let src_idx_next = src_idx + src_step;
        let src_idx_next_int = src_idx_next as usize;
        let src_idx_next_frac = src_idx_next - src_idx_next_int as f32;
        let src_idx_next_frac_inv = 1.0 - src_idx_next_frac;
        let src_idx_frac_inv = 1.0 - src_idx_frac;
        let v = if src_idx_next_int < src_len {
            src[src_idx_int] * src_idx_frac_inv * src_idx_next_frac_inv
                + src[src_idx_next_int] * src_idx_frac * src_idx_next_frac_inv
        } else {
            src[src_idx_int] * src_idx_frac_inv
        };
        *dst = v;
        src_idx += src_step;
    }
}

impl FFTPlayer {
    pub fn new(sample_rate: u32) -> Self {
        Self {
            sliding_window: [0.0; 2048],
            result_buf: [0.0; 2048],
            freq_range: (80.0, 2000.0),
            sample_rate,
        }
    }

    pub fn clear(&mut self) {
        self.sliding_window.fill(0.0);
        self.result_buf.fill(0.0);
    }

    pub fn set_freq_range(&mut self, start_freq: f32, end_freq: f32) {
        self.freq_range = (start_freq, end_freq);
    }

    pub fn push_samples(&mut self, new_samples: &[f32]) {
        let n = new_samples.len();
        if n == 0 {
            return;
        }

        if n >= 2048 {
            self.sliding_window
                .copy_from_slice(&new_samples[n - 2048..n]);
        } else {
            self.sliding_window.copy_within(n..2048, 0);
            self.sliding_window[2048 - n..2048].copy_from_slice(new_samples);
        }
    }

    pub fn read(&mut self, buf: &mut [f32]) -> bool {
        let (start_freq, end_freq) = self.freq_range;
        let fft_window = windows::hamming_window(&self.sliding_window);

        match samples_fft_to_spectrum(
            &fft_window,
            self.sample_rate,
            FrequencyLimit::Range(start_freq, end_freq),
            Some(&scaling::divide_by_N_sqrt),
        ) {
            Ok(spec) => {
                let result_buf_len = self.result_buf.len() as f32;
                let freq_min = spec.min_fr().val();
                let freq_max = spec.max_fr().val();
                let freq_range_val = freq_max - freq_min;
                self.result_buf.iter_mut().enumerate().for_each(|(i, v)| {
                    let freq = i as f32 / result_buf_len * freq_range_val + freq_min;
                    let freq = freq.clamp(freq_min, freq_max);
                    *v += spec.freq_val_exact(freq).val();
                    *v /= 2.0;
                });
                vec_interp(&self.result_buf, buf);
                true
            }
            Err(e) => {
                error!("FFT error: {e:?}");
                false
            }
        }
    }
}
