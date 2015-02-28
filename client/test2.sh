cd fusemount2
ls -l
echo "=== cp ../memory.py . ==="
cp ../memory.py . 
ls -l
echo "=== touch test.py ==="
touch test.py
ls -l --full-time
echo "=== cat * > test.py ==="
cat memory.py > test.py
ls -l
echo "=== ln -s test.py sym_test.py ==="
ln -s test.py sym_test.py
ls -l
echo "=== chmod 755 test.py ==="
chmod 755 test.py
ls -l
echo "=== chown 1000 test.py ==="
chown 1000 test.py
ls -l
echo "=== head test.py ==="
head -n 3 test.py
echo "=== tail test.py ==="
tail -n 3 test.py
echo "=== rm * ==="
rm sym_test.py memory.py test.py
ls -l

