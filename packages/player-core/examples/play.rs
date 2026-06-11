// 一个简单的播放器示例，读取参数中的音频文件并播放

use amll_player_core::{
    AudioPlayer, AudioPlayerConfig, AudioThreadEvent, AudioThreadEventMessage, AudioThreadMessage,
    SongData,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let file_name = std::env::args().nth(1).expect("Usage: play <file>");
    let file_path = std::path::Path::new(&file_name);
    let file_path = if file_path.is_absolute() {
        file_path.to_path_buf()
    } else {
        std::env::current_dir().unwrap().join(file_path)
    };
    let file_path = file_path.to_str().unwrap().to_string();

    let (evt_sender, mut evt_receiver) =
        tokio::sync::mpsc::unbounded_channel::<AudioThreadEventMessage<AudioThreadEvent>>();

    let player = AudioPlayer::new(AudioPlayerConfig {}, evt_sender)?;
    let handler = player.handler();

    handler
        .send_anonymous(AudioThreadMessage::PlayAudio {
            song: SongData::Local { file_path },
        })
        .await?;

    let handler_clone = handler.clone();
    tokio::spawn(async move {
        while let Some(evt) = evt_receiver.recv().await {
            if let Some(evt) = evt.data() {
                match evt {
                    AudioThreadEvent::PlayPosition { position } => {
                        println!("{position:.3}");
                    }
                    AudioThreadEvent::FFTData { .. } => {
                        // 数据量太多就不输出了
                    }
                    AudioThreadEvent::TrackEnded => {
                        println!("播放完成，结束播放");
                        let _ = handler_clone
                            .send_anonymous(AudioThreadMessage::Close)
                            .await;
                        break;
                    }
                    other => {
                        println!("{other:?}");
                    }
                }
            }
        }
    });

    player.run().await;

    Ok(())
}
