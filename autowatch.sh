
while true; do
    # update processing progress
    if [[ $(git diff curr.out) ]]; then 
        echo 'catch changes';
        git add curr.oout;
        git commit -m 'update processing progress';
        git push;
    fi

    # pull new processing list files
    git pull

    # run processing
    if [[ $(ps -u weiz | grep cellSeg.py) ]]; then 
        echo 'There is data in processing'; 
    else
        ./cellSeg.py
    fi
    sleep 3600; # 1 hour a scan
done

