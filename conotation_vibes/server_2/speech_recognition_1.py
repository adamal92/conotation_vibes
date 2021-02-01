import speech_recognition as sr
r = sr.Recognizer()

file = sr.AudioFile('record.wav')
with file as source:
    r.adjust_for_ambient_noise(source)
    audio = r.record(source)
    result = r.recognize_google(audio, language='en')
print(result)

with sr.Microphone() as source:
    r = sr.Recognizer()
    # read the audio data from the default microphone
    audio = r.record(source, duration=4)
    print("Recognizing...")
    # convert speech to text
    # recognize speech using Google Speech Recognition
    try:
        # for testing purposes, we're just using the default API key
        # to use another API key, use `r.recognize_google(audio, key="GOOGLE_SPEECH_RECOGNITION_API_KEY")`
        # instead of `r.recognize_google(audio)`
        print("Google Speech Recognition thinks you said in English: -  " + r.recognize_google(audio, language="en-US"))
    except sr.UnknownValueError:
        print("Google Speech Recognition could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from Google Speech Recognition service; {0}".format(e))
