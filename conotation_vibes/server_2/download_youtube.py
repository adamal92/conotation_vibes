# Web (Extract)
# https://www.youtube.com/watch?v=s7JGOK4UMxY
from pytube import YouTube

# YouTube('https://youtu.be/2lAe1cqCOXo').streams.first().download()
# yt = YouTube('http://youtube.com/watch?v=2lAe1cqCOXo')
# yt.streams \
#     .filter(progressive=True, file_extension='mp4') \
#     .order_by('resolution') \
#     .desc() \
#     .first() \
#     .download()
yt = YouTube('https://www.youtube.com/watch?v=s7JGOK4UMxY')

print(yt.streams.get_audio_only(subtype='mp4'))

yt.streams.get_audio_only(subtype='mp4').download(
    output_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server_2',
    filename='speech', filename_prefix='test'
)

# yt.streams.get_audio_only(subtype='mp4').order_by('resolution') \
#     .desc() \
#     .first() \
#     .download()
