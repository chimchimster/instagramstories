from moviepy.editor import VideoFileClip


class ZipVideoFile:
    def __init__(self, file_path: str, filename: str) -> None:
        self.file_path = file_path
        self.filename = filename

    def resize_video_file(self):
        video_clip = VideoFileClip(self.file_path)

        # Resizing old video
        new_video = video_clip.resize(0.5)

        # Saving video
        new_video.write_videofile(self.file_path[:-4] + '_vid.mp4', fps=30)

        return self.filename[:-4] + '_vid.mp4'