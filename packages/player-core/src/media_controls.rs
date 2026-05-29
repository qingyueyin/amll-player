use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tracing::warn;

use crate::{
    AudioInfo, AudioPlayerEventSender, AudioPlayerHandle, AudioThreadEvent,
    AudioThreadEventMessage, AudioThreadMessage,
};
use now_playing_controls::model::{
    MetadataPayload, PlayStatePayload, PlaybackStatus, SystemMediaEvent, SystemMediaEventType,
    TimelinePayload,
};

pub struct SystemMediaManager {}

impl SystemMediaManager {
    pub fn new() -> (Self, Option<UnboundedReceiver<SystemMediaEvent>>) {
        let npc_event_rx = match now_playing_controls::initialize() {
            Ok(()) => {
                let (npc_tx, npc_rx) = tokio::sync::mpsc::unbounded_channel();
                if let Err(e) = now_playing_controls::register_event_handler(Arc::new(
                    move |event: SystemMediaEvent| {
                        let _ = npc_tx.send(event);
                    },
                )) {
                    warn!("注册系统媒体控件事件处理器失败：{e:?}");
                }
                if let Err(e) = now_playing_controls::enable_system_media() {
                    warn!("启用系统媒体控件失败：{e:?}");
                }
                Some(npc_rx)
            }
            Err(err) => {
                warn!("初始化系统媒体控件时出错：{err:?}");
                None
            }
        };

        (Self {}, npc_event_rx)
    }

    pub fn update_metadata(&self, audio_info: &AudioInfo) {
        now_playing_controls::update_metadata(MetadataPayload {
            song_name: audio_info.name.clone(),
            author_name: audio_info.artist.clone(),
            album_name: audio_info.album.clone(),
            cover_data: audio_info.cover.clone(),
            original_cover_url: None,
            genre: Vec::new(),
            track_id: None,
            discord_button_url: None,
            duration: Some(audio_info.duration * 1000.0),
        });
    }

    pub fn update_play_state(&self, is_playing: bool) {
        now_playing_controls::update_play_state(PlayStatePayload {
            status: if is_playing {
                PlaybackStatus::Playing
            } else {
                PlaybackStatus::Paused
            },
        });
    }

    pub fn update_timeline(&self, current_time_sec: f64, total_time_sec: f64) {
        now_playing_controls::update_timeline(TimelinePayload {
            current_time: current_time_sec * 1000.0,
            total_time: total_time_sec * 1000.0,
            seeked: None,
        });
    }

    pub fn update_playback_rate(&self, rate: f64) {
        now_playing_controls::update_playback_rate(rate);
    }

    pub fn set_enabled(&self, enabled: bool) {
        if enabled {
            if let Err(e) = now_playing_controls::enable_system_media() {
                warn!("启用系统媒体控件失败: {e:?}");
            }
        } else {
            if let Err(e) = now_playing_controls::disable_system_media() {
                warn!("禁用系统媒体控件失败: {e:?}");
            }
        }
    }

    pub async fn handle_event(
        &self,
        event: SystemMediaEvent,
        player_handler: &AudioPlayerHandle,
        event_sender: &AudioPlayerEventSender,
    ) {
        let result = match event.type_ {
            SystemMediaEventType::Play => {
                player_handler
                    .send_anonymous(AudioThreadMessage::ResumeAudio)
                    .await
            }
            SystemMediaEventType::Pause => {
                player_handler
                    .send_anonymous(AudioThreadMessage::PauseAudio)
                    .await
            }
            SystemMediaEventType::NextSong => {
                let evt = AudioThreadEventMessage::new(
                    "".into(),
                    Some(AudioThreadEvent::HardwareMediaCommand {
                        command: "next".into(),
                    }),
                );
                event_sender.send(evt).map_err(anyhow::Error::from)
            }
            SystemMediaEventType::PreviousSong => {
                let evt = AudioThreadEventMessage::new(
                    "".into(),
                    Some(AudioThreadEvent::HardwareMediaCommand {
                        command: "prev".into(),
                    }),
                );
                event_sender.send(evt).map_err(anyhow::Error::from)
            }
            SystemMediaEventType::Seek => {
                if let Some(pos_ms) = event.position_ms {
                    player_handler
                        .send_anonymous(AudioThreadMessage::SeekAudio {
                            position: pos_ms / 1000.0,
                        })
                        .await
                } else {
                    Ok(())
                }
            }
            SystemMediaEventType::Stop => {
                player_handler
                    .send_anonymous(AudioThreadMessage::StopAudio)
                    .await
            }
            SystemMediaEventType::ToggleShuffle => {
                player_handler
                    .send_anonymous(AudioThreadMessage::ToggleShuffle)
                    .await
            }
            SystemMediaEventType::ToggleRepeat => {
                player_handler
                    .send_anonymous(AudioThreadMessage::ToggleRepeat)
                    .await
            }
            SystemMediaEventType::SetRate => {
                if let Some(rate) = event.rate {
                    player_handler
                        .send_anonymous(AudioThreadMessage::SetPlaybackRate { rate })
                        .await
                } else {
                    Ok(())
                }
            }
            SystemMediaEventType::SetVolume => {
                if let Some(volume) = event.volume {
                    player_handler
                        .send_anonymous(AudioThreadMessage::SetVolume { volume })
                        .await
                } else {
                    Ok(())
                }
            }
        };

        if let Err(e) = result {
            warn!("处理系统媒体控件事件失败: {e:?}");
        }
    }
}

impl Drop for SystemMediaManager {
    fn drop(&mut self) {
        now_playing_controls::shutdown();
    }
}
